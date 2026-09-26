import { computed, inject, Injectable, Service, signal } from '@angular/core';
import { environment } from '../../../environments/environment';
import { HttpClient } from '@angular/common/http';
import { Cart, CartItem } from '../../shared/Models/cart';
import { Product } from '../../shared/Models/product';
import { firstValueFrom, map } from 'rxjs';
import { DeliveryMethod } from '../../shared/Models/deliveryMethod';

@Injectable({
  providedIn: 'root'
})
export class CartService {
    baseUrl = environment.apiUrl
    private http = inject(HttpClient)
    cart = signal<Cart | null>(null);
    selectedDelivery = signal<DeliveryMethod | null>(null);
    itemCount = computed(() => {
        return this.cart()?.cartItems.reduce((sum, item) => sum + item.quantity, 0)
    })

    totals = computed(() => {
        const cart = this.cart();
        const delivery = this.selectedDelivery();

        if (!cart) return null;
        const subtotal = cart.cartItems.reduce((sum, item) => sum + item.price * item.quantity, 0);

        let discountValue = 0;

        const shipping = delivery ? delivery.price : 0;

        const total = subtotal + shipping - discountValue

        return {
            subtotal,
            shipping,
            discount: discountValue,
            total
        };
    })

    getCart(id: string)
    {
        return this.http.get<Cart>(this.baseUrl + 'cart?id='+id).pipe(
            map(cart => {
                this.cart.set(cart);
                return cart
            })
        )
    }

    setCart(cartData: Cart)
    {
        return this.http.post<Cart>(this.baseUrl + 'cart', cartData).subscribe({
                next: cart => this.cart.set(cart)
            }
        )
    }

    deleteCart() {
        this.http.delete(this.baseUrl + 'cart?id=' + this.cart()?.id).subscribe({
        next: () => {
            localStorage.removeItem('cart_id');
            this.cart.set(null);
        }
        });
    }

    addItemsToCart(item: CartItem | Product, quantity = 1)
    {
        const createCart = this.cart() ?? this.CreateCart()
        if (this.isProduct(item))
        {
            item = this.mapProductToCart(item);
        }

        createCart.cartItems = this.addOrUpdateCartItem(createCart.cartItems, item, quantity);

        this.setCart(createCart);
    }

    removeItemFromCart(productId: number, quantity = 1) {
        const cart = this.cart();
        if (!cart) return;
        const index = cart.cartItems.findIndex(i => i.productId === productId);
        if (index !== -1) {
            if (cart.cartItems[index].quantity > quantity) {
                cart.cartItems[index].quantity -= quantity;
            } 
            else {
                cart.cartItems.splice(index, 1);
            }
            if (cart.cartItems.length === 0) {
                this.deleteCart();
            } else {
                this.setCart(cart);
            }
        }
  }

    private addOrUpdateCartItem(cartItems: CartItem[], item: CartItem, quantity: number): CartItem[]
    {
        if (cartItems.length > 0)
        {
            const index = cartItems.findIndex(x=> x.productId == item.productId);
            if (index == -1)
            {
                item.quantity = quantity;
                cartItems.push(item);      
            }
            else
            {
                cartItems[index].quantity += quantity
            }
        }
        else
        {
            item.quantity = quantity;
            cartItems.push(item);  
        }
        

        return cartItems;
    }

    private mapProductToCart(product: Product): CartItem
    {
        return {
            productId: product.id,
            productName: product.name,
            price: product.price,
            quantity: 0,
            pictureUrl: product.pictureUrl,
            brand: product.brand,
            type: product.type  
        };
    }

    private isProduct(item: CartItem | Product): item is Product
    {
        return (item as Product).id !== undefined;
    }

    private CreateCart() : Cart
    {
        const cart = new Cart();
        localStorage.setItem("cart_id", cart.id)
        return cart;
    }
}
