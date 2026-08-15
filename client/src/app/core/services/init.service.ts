import { inject, Service } from '@angular/core';
import { CartService } from './cart.service';
import { forkJoin, of } from 'rxjs';
import { AccountService } from './account.service';

@Service()
export class InitService {
    private cartService = inject(CartService)
    private accService = inject(AccountService)
;
    init() {
        const cart_id = localStorage.getItem('cart_id');
        const carts = cart_id ?  this.cartService.getCart(cart_id) : of(null);

        return forkJoin(
            {
                cart: carts,
                userInfo: this.accService.getUserInfo()
            }
        )
    }
}
