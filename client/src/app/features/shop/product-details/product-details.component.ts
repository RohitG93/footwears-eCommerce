import { ChangeDetectorRef, Component, inject, OnInit } from '@angular/core';
import { Product } from '../../../shared/Models/product';
import { ShopService } from '../../../core/services/shop.service';
import { ActivatedRoute } from '@angular/router';
import { CurrencyPipe } from '@angular/common';
import { MatIcon } from '@angular/material/icon';
import { MatButton } from '@angular/material/button';
import { MatFormField, MatInput, MatLabel } from '@angular/material/input';
import { MatDivider } from '@angular/material/list';
import { CartService } from '../../../core/services/cart.service';
import { FormsModule } from '@angular/forms';
  
@Component({
  selector: 'app-product-details',
  imports: [
    CurrencyPipe,
    MatButton,
    MatIcon,
    MatFormField,
    MatInput,
    MatLabel,
    MatDivider,
    FormsModule
  ],
  templateUrl: './product-details.component.html',
  styleUrl: './product-details.component.scss',
})
export class ProductDetailsComponent implements OnInit {

  private shopservice = inject(ShopService);
  private activatedRoute = inject(ActivatedRoute);
  cartService = inject(CartService)
  product: Product | null = null;
  private cdr = inject(ChangeDetectorRef);
  quantityInCart=0
  quantity=1

  ngOnInit(): void {
    this.loadProduct();
  }

  loadProduct() {
    const id = this.activatedRoute.snapshot.paramMap.get('id');
    if (id) {
      this.shopservice.getProductById(+id).subscribe({
        next: (product) => {
          this.product = product;
          this.updateQuantityInBasket();
          this.cdr.detectChanges();
        },
        error: (error) => {
          console.error('Error fetching product details:', error);
        }
      });
    }
  }

  updateCart() {
    if (!this.product) return;
    if (this.quantity > this.quantityInCart) {
      const itemsToAdd = this.quantity - this.quantityInCart;
      this.quantityInCart += itemsToAdd;
      this.cartService.addItemsToCart(this.product, itemsToAdd);
    } else {
      const itemsToRemove = this.quantityInCart - this.quantity;
      this.quantityInCart -= itemsToRemove;
      this.cartService.removeItemFromCart(this.product.id, itemsToRemove);
    }
  }

  updateQuantityInBasket() {
    this.quantityInCart = this.cartService.cart()?.cartItems.find(item => item.productId === this.product?.id)?.quantity || 0;
    this.quantity = this.quantityInCart || 1;
  }

  getButtonText() {
    return this.quantityInCart > 0 ? 'Update Cart' : 'Add to Cart';
  }
}
